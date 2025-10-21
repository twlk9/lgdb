package compression

import (
	"testing"

	"github.com/klauspost/compress/snappy"
)

func TestCompressionTypes(t *testing.T) {
	tests := []struct {
		compType Type
		expected string
	}{
		{None, "none"},
		{Snappy, "snappy"},
		{Zstd, "zstd"},
		{S2, "s2"},
		{Type(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.compType.String(); got != tt.expected {
			t.Errorf("Type.String() = %v, want %v", got, tt.expected)
		}
	}
}

func TestNoCompression(t *testing.T) {
	config := Config{Type: None, MinReductionPercent: 0}
	compressor, err := NewCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	if compressor.Type() != None {
		t.Errorf("Expected compression type %v, got %v", None, compressor.Type())
	}

	// Test compression
	src := []byte("Hello, World!")
	dst := make([]byte, 0, len(src))

	compressed, wasCompressed, err := compressor.Compress(dst, src)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	if wasCompressed {
		t.Error("No compression should not report as compressed")
	}

	if string(compressed) != string(src) {
		t.Errorf("No compression should return original data, got %v", compressed)
	}

	// Test decompression
	decompressed, err := compressor.Decompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompression failed: %v", err)
	}

	if string(decompressed) != string(src) {
		t.Errorf("Decompressed data doesn't match original: got %v, want %v", decompressed, src)
	}
}

func TestSnappyCompression(t *testing.T) {
	config := Config{Type: Snappy, MinReductionPercent: 0}
	compressor, err := NewCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	if compressor.Type() != Snappy {
		t.Errorf("Expected compression type %v, got %v", Snappy, compressor.Type())
	}

	// Test with compressible data
	src := make([]byte, 1000)
	for i := range src {
		src[i] = byte(i % 10) // Highly compressible pattern
	}

	compressed, wasCompressed, err := compressor.Compress(nil, src)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	if !wasCompressed {
		t.Error("Snappy compression should report as compressed for compressible data")
	}

	if len(compressed) >= len(src) {
		t.Errorf("Compressed data should be smaller than original: got %d, original %d", len(compressed), len(src))
	}

	// Test decompression
	decompressed, err := compressor.Decompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompression failed: %v", err)
	}

	if len(decompressed) != len(src) {
		t.Errorf("Decompressed data length mismatch: got %d, want %d", len(decompressed), len(src))
	}

	for i := range src {
		if decompressed[i] != src[i] {
			t.Errorf("Decompressed data mismatch at index %d: got %v, want %v", i, decompressed[i], src[i])
		}
	}
}

func TestSnappyMinReductionThreshold(t *testing.T) {
	// Set high minimum reduction threshold
	config := Config{Type: Snappy, MinReductionPercent: 50}
	compressor, err := NewCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	// Use truly random data that won't compress well
	src := make([]byte, 100)
	for i := range src {
		src[i] = byte(i ^ (i * 7) ^ (i * 13)) // Pseudo-random pattern
	}

	compressed, wasCompressed, err := compressor.Compress(nil, src)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	// Check if this data actually compresses poorly
	testCompressed := snappy.Encode(nil, src)
	reductionPercent := (len(src) - len(testCompressed)) * 100 / len(src)

	t.Logf("Original size: %d, compressed size: %d, reduction: %d%%", len(src), len(testCompressed), reductionPercent)

	if reductionPercent >= 50 {
		t.Skipf("Test data compressed too well (%d%% reduction), skipping threshold test", reductionPercent)
	}

	if wasCompressed {
		t.Errorf("Compression should be skipped when reduction threshold is not met (got %d%% reduction, threshold %d%%)", reductionPercent, 50)
	}

	if string(compressed) != string(src) {
		t.Errorf("When compression is skipped, should return original data")
	}
}

func TestCompressBlock(t *testing.T) {
	config := Config{Type: Snappy, MinReductionPercent: 0}
	compressor, err := NewCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	// Test with compressible data (above 1024 byte threshold)
	src := make([]byte, 2000) // Increased size to exceed 1KB threshold
	for i := range src {
		src[i] = byte('A') // Highly compressible
	}

	compressed, compressionType, err := CompressBlock(compressor, nil, src)
	if err != nil {
		t.Fatalf("CompressBlock failed: %v", err)
	}

	if compressionType != BlockSnappy {
		t.Errorf("Expected compression type %v, got %v", BlockSnappy, compressionType)
	}

	if len(compressed) >= len(src) {
		t.Errorf("Compressed block should be smaller: got %d, original %d", len(compressed), len(src))
	}
}

func TestCompressBlockThreshold(t *testing.T) {
	config := Config{Type: Snappy, MinReductionPercent: 0}
	compressor, err := NewCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	// Test with small data (below 1024 byte threshold)
	src := make([]byte, 500) // Below 1KB threshold
	for i := range src {
		src[i] = byte('A') // Even highly compressible data should be skipped
	}

	compressed, compressionType, err := CompressBlock(compressor, nil, src)
	if err != nil {
		t.Fatalf("CompressBlock failed: %v", err)
	}

	// Should skip compression due to size threshold
	if compressionType != BlockNone {
		t.Errorf("Expected compression type %v for small block, got %v", BlockNone, compressionType)
	}

	if string(compressed) != string(src) {
		t.Errorf("Small block should be returned uncompressed")
	}
}

func TestDecompressBlock(t *testing.T) {
	// Test no compression
	src := []byte("Hello, World!")
	decompressed, err := DecompressBlock(nil, src, BlockNone)
	if err != nil {
		t.Fatalf("DecompressBlock failed for no compression: %v", err)
	}

	if string(decompressed) != string(src) {
		t.Errorf("DecompressBlock with no compression should return original data")
	}

	// Test Snappy compression
	config := Config{Type: Snappy, MinReductionPercent: 0}
	compressor, err := NewCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	// Create compressible data
	original := make([]byte, 1000)
	for i := range original {
		original[i] = byte('A')
	}

	// Compress it
	compressed, wasCompressed, err := compressor.Compress(nil, original)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	if !wasCompressed {
		t.Fatal("Expected data to be compressed")
	}

	// Decompress using DecompressBlock
	decompressed, err = DecompressBlock(nil, compressed, BlockSnappy)
	if err != nil {
		t.Fatalf("DecompressBlock failed for Snappy: %v", err)
	}

	if len(decompressed) != len(original) {
		t.Errorf("Decompressed length mismatch: got %d, want %d", len(decompressed), len(original))
	}

	for i := range original {
		if decompressed[i] != original[i] {
			t.Errorf("Decompressed data mismatch at index %d", i)
		}
	}
}

func TestInvalidCompressionType(t *testing.T) {
	// Test invalid compression type in config
	config := Config{Type: Type(99), MinReductionPercent: 0}
	_, err := NewCompressor(config)
	if err == nil {
		t.Error("Expected error for invalid compression type")
	}

	// Test invalid compression type in DecompressBlock
	_, err = DecompressBlock(nil, []byte("test"), 99)
	if err == nil {
		t.Error("Expected error for invalid block compression type")
	}
}

func TestZstdCompression(t *testing.T) {
	testCases := []struct {
		name   string
		level  ZstdLevel
		config Config
	}{
		{
			name:   "ZstdFastest",
			level:  ZstdFastest,
			config: Config{Type: Zstd, MinReductionPercent: 0, ZstdLevel: ZstdFastest},
		},
		{
			name:   "ZstdDefault",
			level:  ZstdDefault,
			config: Config{Type: Zstd, MinReductionPercent: 0, ZstdLevel: ZstdDefault},
		},
		{
			name:   "ZstdBetter",
			level:  ZstdBetter,
			config: Config{Type: Zstd, MinReductionPercent: 0, ZstdLevel: ZstdBetter},
		},
		{
			name:   "ZstdBest",
			level:  ZstdBest,
			config: Config{Type: Zstd, MinReductionPercent: 0, ZstdLevel: ZstdBest},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compressor, err := NewCompressor(tc.config)
			if err != nil {
				t.Fatalf("Failed to create Zstd compressor: %v", err)
			}

			if compressor.Type() != Zstd {
				t.Errorf("Expected compression type %v, got %v", Zstd, compressor.Type())
			}

			// Test with compressible data
			src := make([]byte, 1000)
			for i := range src {
				src[i] = byte(i % 10) // Highly compressible pattern
			}

			compressed, wasCompressed, err := compressor.Compress(nil, src)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			if !wasCompressed {
				t.Error("Zstd compression should report as compressed for compressible data")
			}

			if len(compressed) >= len(src) {
				t.Errorf("Compressed data should be smaller than original: got %d, original %d", len(compressed), len(src))
			}

			// Test decompression
			decompressed, err := compressor.Decompress(nil, compressed)
			if err != nil {
				t.Fatalf("Decompression failed: %v", err)
			}

			if len(decompressed) != len(src) {
				t.Errorf("Decompressed data length mismatch: got %d, want %d", len(decompressed), len(src))
			}

			for i := range src {
				if decompressed[i] != src[i] {
					t.Errorf("Decompressed data mismatch at index %d: got %v, want %v", i, decompressed[i], src[i])
				}
			}
		})
	}
}

func TestZstdMinReductionThreshold(t *testing.T) {
	// Set high minimum reduction threshold
	config := Config{Type: Zstd, MinReductionPercent: 50, ZstdLevel: ZstdDefault}
	compressor, err := NewCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	// Use truly random data that won't compress well
	src := make([]byte, 100)
	for i := range src {
		src[i] = byte(i ^ (i * 7) ^ (i * 13)) // Pseudo-random pattern
	}

	compressed, wasCompressed, err := compressor.Compress(nil, src)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	// Check if this data actually compresses poorly with Zstd
	testCompressor, _ := NewCompressor(Config{Type: Zstd, MinReductionPercent: 0, ZstdLevel: ZstdDefault})
	testCompressed, _, _ := testCompressor.Compress(nil, src)
	reductionPercent := (len(src) - len(testCompressed)) * 100 / len(src)

	t.Logf("Original size: %d, compressed size: %d, reduction: %d%%", len(src), len(testCompressed), reductionPercent)

	if reductionPercent >= 50 {
		t.Skipf("Test data compressed too well (%d%% reduction), skipping threshold test", reductionPercent)
	}

	if wasCompressed {
		t.Errorf("Compression should be skipped when reduction threshold is not met (got %d%% reduction, threshold %d%%)", reductionPercent, 50)
	}

	if string(compressed) != string(src) {
		t.Errorf("When compression is skipped, should return original data")
	}
}

func TestZstdConfigFunctions(t *testing.T) {
	testCases := []struct {
		name          string
		configFunc    func() Config
		expectedType  Type
		expectedLevel ZstdLevel
	}{
		{"ZstdFastConfig", ZstdFastConfig, Zstd, ZstdFastest},
		{"ZstdBalancedConfig", ZstdBalancedConfig, Zstd, ZstdDefault},
		{"ZstdBestConfig", ZstdBestConfig, Zstd, ZstdBest},
		{"SnappyConfig", SnappyConfig, Snappy, ZstdDefault},             // Level doesn't matter for Snappy
		{"NoCompressionConfig", NoCompressionConfig, None, ZstdDefault}, // Level doesn't matter for None
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := tc.configFunc()

			if config.Type != tc.expectedType {
				t.Errorf("Expected type %v, got %v", tc.expectedType, config.Type)
			}

			if config.Type == Zstd && config.ZstdLevel != tc.expectedLevel {
				t.Errorf("Expected Zstd level %v, got %v", tc.expectedLevel, config.ZstdLevel)
			}

			// Test that we can create a compressor with this config
			compressor, err := NewCompressor(config)
			if err != nil {
				t.Fatalf("Failed to create compressor with config: %v", err)
			}

			if compressor.Type() != tc.expectedType {
				t.Errorf("Compressor type mismatch: expected %v, got %v", tc.expectedType, compressor.Type())
			}
		})
	}
}

func TestZstdDecompressBlock(t *testing.T) {
	// Test Zstd decompression in DecompressBlock function
	config := Config{Type: Zstd, MinReductionPercent: 0, ZstdLevel: ZstdDefault}
	compressor, err := NewCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	// Create compressible data
	original := make([]byte, 1000)
	for i := range original {
		original[i] = byte('A')
	}

	// Compress it
	compressed, wasCompressed, err := compressor.Compress(nil, original)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	if !wasCompressed {
		t.Fatal("Expected data to be compressed")
	}

	// Decompress using DecompressBlock
	decompressed, err := DecompressBlock(nil, compressed, BlockZstd)
	if err != nil {
		t.Fatalf("DecompressBlock failed for Zstd: %v", err)
	}

	if len(decompressed) != len(original) {
		t.Errorf("Decompressed length mismatch: got %d, want %d", len(decompressed), len(original))
	}

	for i := range original {
		if decompressed[i] != original[i] {
			t.Errorf("Decompressed data mismatch at index %d", i)
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.Type != S2 {
		t.Errorf("Default compression type should be S2, got %v", config.Type)
	}

	if config.MinReductionPercent != 12 {
		t.Errorf("Default min reduction percent should be 12, got %v", config.MinReductionPercent)
	}
}

func TestS2Compression(t *testing.T) {
	config := Config{Type: S2, MinReductionPercent: 0}
	compressor, err := NewCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	if compressor.Type() != S2 {
		t.Errorf("Expected compression type %v, got %v", S2, compressor.Type())
	}

	// Test with compressible data
	src := make([]byte, 1000)
	for i := range src {
		src[i] = byte(i % 10) // Highly compressible pattern
	}

	compressed, wasCompressed, err := compressor.Compress(nil, src)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	if !wasCompressed {
		t.Error("S2 compression should report as compressed for compressible data")
	}

	if len(compressed) >= len(src) {
		t.Errorf("Compressed data should be smaller than original: got %d, original %d", len(compressed), len(src))
	}

	// Test decompression
	decompressed, err := compressor.Decompress(nil, compressed)
	if err != nil {
		t.Fatalf("Decompression failed: %v", err)
	}

	if len(decompressed) != len(src) {
		t.Errorf("Decompressed data length mismatch: got %d, want %d", len(decompressed), len(src))
	}

	for i := range src {
		if decompressed[i] != src[i] {
			t.Errorf("Decompressed data mismatch at index %d: got %v, want %v", i, decompressed[i], src[i])
		}
	}
}

func TestS2MinReductionThreshold(t *testing.T) {
	// Set high minimum reduction threshold
	config := Config{Type: S2, MinReductionPercent: 50}
	compressor, err := NewCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	// Use truly random data that won't compress well
	src := make([]byte, 100)
	for i := range src {
		src[i] = byte(i ^ (i * 7) ^ (i * 13)) // Pseudo-random pattern
	}

	compressed, wasCompressed, err := compressor.Compress(nil, src)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	// Check if this data actually compresses poorly with S2
	testCompressor, _ := NewCompressor(Config{Type: S2, MinReductionPercent: 0})
	testCompressed, _, _ := testCompressor.Compress(nil, src)
	reductionPercent := (len(src) - len(testCompressed)) * 100 / len(src)

	t.Logf("Original size: %d, compressed size: %d, reduction: %d%%", len(src), len(testCompressed), reductionPercent)

	if reductionPercent >= 50 {
		t.Skipf("Test data compressed too well (%d%% reduction), skipping threshold test", reductionPercent)
	}

	if wasCompressed {
		t.Errorf("Compression should be skipped when reduction threshold is not met (got %d%% reduction, threshold %d%%)", reductionPercent, 50)
	}

	if string(compressed) != string(src) {
		t.Errorf("When compression is skipped, should return original data")
	}
}

func TestS2ConfigFunctions(t *testing.T) {
	testCases := []struct {
		name         string
		configFunc   func() Config
		expectedType Type
	}{
		{"S2DefaultConfig", S2DefaultConfig, S2},
		{"S2BetterConfig", S2BetterConfig, S2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := tc.configFunc()

			if config.Type != tc.expectedType {
				t.Errorf("Expected type %v, got %v", tc.expectedType, config.Type)
			}

			// Test that we can create a compressor with this config
			compressor, err := NewCompressor(config)
			if err != nil {
				t.Fatalf("Failed to create compressor with config: %v", err)
			}

			if compressor.Type() != tc.expectedType {
				t.Errorf("Compressor type mismatch: expected %v, got %v", tc.expectedType, compressor.Type())
			}
		})
	}
}

func TestS2DecompressBlock(t *testing.T) {
	// Test S2 decompression in DecompressBlock function
	config := Config{Type: S2, MinReductionPercent: 0}
	compressor, err := NewCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	// Create compressible data
	original := make([]byte, 1000)
	for i := range original {
		original[i] = byte('A')
	}

	// Compress it
	compressed, wasCompressed, err := compressor.Compress(nil, original)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	if !wasCompressed {
		t.Fatal("Expected data to be compressed")
	}

	// Decompress using DecompressBlock
	decompressed, err := DecompressBlock(nil, compressed, BlockS2)
	if err != nil {
		t.Fatalf("DecompressBlock failed for S2: %v", err)
	}

	if len(decompressed) != len(original) {
		t.Errorf("Decompressed length mismatch: got %d, want %d", len(decompressed), len(original))
	}

	for i := range original {
		if decompressed[i] != original[i] {
			t.Errorf("Decompressed data mismatch at index %d", i)
		}
	}
}

func TestS2CompressBlock(t *testing.T) {
	config := Config{Type: S2, MinReductionPercent: 0}
	compressor, err := NewCompressor(config)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	// Test with compressible data (above 1024 byte threshold)
	src := make([]byte, 2000)
	for i := range src {
		src[i] = byte('A') // Highly compressible
	}

	compressed, compressionType, err := CompressBlock(compressor, nil, src)
	if err != nil {
		t.Fatalf("CompressBlock failed: %v", err)
	}

	if compressionType != BlockS2 {
		t.Errorf("Expected compression type %v, got %v", BlockS2, compressionType)
	}

	if len(compressed) >= len(src) {
		t.Errorf("Compressed block should be smaller: got %d, original %d", len(compressed), len(src))
	}
}

func TestTieredCompressionLevelSelection(t *testing.T) {
	tc := DefaultTieredConfig()

	// Verify default config uses S2 for top levels
	if tc.TopCompression.Type != S2 {
		t.Errorf("DefaultTieredConfig should use S2 for TopCompression, got %v", tc.TopCompression.Type)
	}

	// Verify default config uses Zstd for bottom levels
	if tc.BottomCompression.Type != Zstd {
		t.Errorf("DefaultTieredConfig should use Zstd for BottomCompression, got %v", tc.BottomCompression.Type)
	}

	// Verify TopLevelCount is 3
	if tc.TopLevelCount != 3 {
		t.Errorf("DefaultTieredConfig should have TopLevelCount=3, got %d", tc.TopLevelCount)
	}

	// Test top levels (L0, L1, L2)
	for i := 0; i < 3; i++ {
		cfg := tc.GetConfigForLevel(i)
		if cfg.Type != S2 {
			t.Errorf("Level %d should use S2, got %v", i, cfg.Type)
		}
	}

	// Test bottom levels (L3+)
	for i := 3; i < 7; i++ {
		cfg := tc.GetConfigForLevel(i)
		if cfg.Type != Zstd {
			t.Errorf("Level %d should use Zstd, got %v", i, cfg.Type)
		}
	}
}

func TestUniformFastConfig(t *testing.T) {
	tc := UniformFastConfig()

	// Verify uses S2 for both top and bottom
	if tc.TopCompression.Type != S2 {
		t.Errorf("UniformFastConfig should use S2 for TopCompression, got %v", tc.TopCompression.Type)
	}
	if tc.BottomCompression.Type != S2 {
		t.Errorf("UniformFastConfig should use S2 for BottomCompression, got %v", tc.BottomCompression.Type)
	}

	// All levels should use same compression
	topCfg := tc.GetConfigForLevel(0)
	bottomCfg := tc.GetConfigForLevel(10)

	if topCfg.Type != bottomCfg.Type {
		t.Errorf("Uniform config should use same type at all levels: top=%v, bottom=%v", topCfg.Type, bottomCfg.Type)
	}
	if topCfg.Type != S2 {
		t.Errorf("UniformFastConfig should use S2 at all levels, got %v", topCfg.Type)
	}
}

func TestUniformBestConfig(t *testing.T) {
	tc := UniformBestConfig()

	// Verify uses Zstd for both top and bottom
	if tc.TopCompression.Type != Zstd {
		t.Errorf("UniformBestConfig should use Zstd for TopCompression, got %v", tc.TopCompression.Type)
	}
	if tc.BottomCompression.Type != Zstd {
		t.Errorf("UniformBestConfig should use Zstd for BottomCompression, got %v", tc.BottomCompression.Type)
	}

	// All levels should use Zstd
	topCfg := tc.GetConfigForLevel(0)
	bottomCfg := tc.GetConfigForLevel(10)

	if topCfg.Type != bottomCfg.Type {
		t.Errorf("Uniform config should use same type at all levels: top=%v, bottom=%v", topCfg.Type, bottomCfg.Type)
	}
	if topCfg.Type != Zstd {
		t.Errorf("UniformBestConfig should use Zstd at all levels, got %v", topCfg.Type)
	}
}

func TestAggressiveTieredConfig(t *testing.T) {
	tc := AggressiveTieredConfig()

	// Verify uses None for top levels
	if tc.TopCompression.Type != None {
		t.Errorf("AggressiveTieredConfig should use None for TopCompression, got %v", tc.TopCompression.Type)
	}

	// Verify uses ZstdBest for bottom levels
	if tc.BottomCompression.Type != Zstd {
		t.Errorf("AggressiveTieredConfig should use Zstd for BottomCompression, got %v", tc.BottomCompression.Type)
	}

	// Test top levels should have no compression
	for i := 0; i < 3; i++ {
		cfg := tc.GetConfigForLevel(i)
		if cfg.Type != None {
			t.Errorf("Level %d should use None, got %v", i, cfg.Type)
		}
	}

	// Test bottom levels should have Zstd compression
	for i := 3; i < 7; i++ {
		cfg := tc.GetConfigForLevel(i)
		if cfg.Type != Zstd {
			t.Errorf("Level %d should use Zstd, got %v", i, cfg.Type)
		}
	}
}

func TestTieredCompressionBoundaryConditions(t *testing.T) {
	tests := []struct {
		name          string
		topLevelCount int
		testLevel     int
		expectTop     bool
	}{
		{"Level 0 with boundary 1", 1, 0, true},
		{"Level 1 with boundary 1", 1, 1, false},
		{"Level 0 with boundary 0", 0, 0, false},
		{"Level 5 with boundary 5", 5, 4, true},
		{"Level 5 with boundary 5", 5, 5, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := TieredCompressionConfig{
				TopCompression:    S2DefaultConfig(),
				BottomCompression: ZstdBalancedConfig(),
				TopLevelCount:     tt.topLevelCount,
			}

			cfg := tc.GetConfigForLevel(tt.testLevel)
			isTop := cfg.Type == S2

			if isTop != tt.expectTop {
				t.Errorf("Level %d with TopLevelCount=%d: expected top=%v, got top=%v",
					tt.testLevel, tt.topLevelCount, tt.expectTop, isTop)
			}
		})
	}
}
