package compression

import (
	"fmt"

	"github.com/klauspost/compress/s2"
)

// s2Compressor implements S2 compression
type s2Compressor struct {
	minReductionPercent uint8
}

// NewS2Compressor creates a new S2 compressor
func NewS2Compressor(minReductionPercent uint8) Compressor {
	return &s2Compressor{
		minReductionPercent: minReductionPercent,
	}
}

func (c *s2Compressor) Compress(dst, src []byte) ([]byte, bool, error) {
	// Compress using S2
	compressed := s2.Encode(dst, src)

	// Check if compression meets minimum reduction threshold
	if c.minReductionPercent > 0 {
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

func (c *s2Compressor) Decompress(dst, src []byte) ([]byte, error) {
	decompressed, err := s2.Decode(dst, src)
	if err != nil {
		return nil, fmt.Errorf("s2 decompression failed: %w", err)
	}
	return decompressed, nil
}

func (c *s2Compressor) Type() Type {
	return S2
}

// DecompressS2 decompresses S2-compressed data
// This function can be used directly when you know the compression type
func DecompressS2(dst, src []byte) ([]byte, error) {
	decompressed, err := s2.Decode(dst, src)
	if err != nil {
		return nil, fmt.Errorf("s2 decompression failed: %w", err)
	}
	return decompressed, nil
}
