package lgdb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileCacheSize(t *testing.T) {
	tests := []struct {
		name         string
		maxOpenFiles int
		expected     int
	}{
		{
			name:         "normal case",
			maxOpenFiles: 1000,
			expected:     990, // 1000 - 10 reserved
		},
		{
			name:         "small maxOpenFiles",
			maxOpenFiles: 50,
			expected:     64, // Uses MinFileCacheSize due to being too small
		},
		{
			name:         "exactly minimum",
			maxOpenFiles: 74, // 64 + 10 = 74
			expected:     64,
		},
		{
			name:         "very large maxOpenFiles",
			maxOpenFiles: 10000,
			expected:     9990, // 10000 - 10 reserved
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FileCacheSize(tt.maxOpenFiles)
			if result != tt.expected {
				t.Errorf("FileCacheSize(%d) = %d, expected %d", tt.maxOpenFiles, result, tt.expected)
			}
		})
	}
}

func TestDefaultOptionsFileCacheSize(t *testing.T) {
	opts := DefaultOptions()
	expectedCacheSize := FileCacheSize(DefaultMaxOpenFiles)

	if opts.GetFileCacheSize() != expectedCacheSize {
		t.Errorf("DefaultOptions GetFileCacheSize() = %d, expected %d",
			opts.GetFileCacheSize(), expectedCacheSize)
	}

	// Verify the calculation is correct
	if expectedCacheSize != 990 { // 1000 - 10
		t.Errorf("Expected default cache size to be 990, got %d", expectedCacheSize)
	}
}

func TestGetFileCacheSize(t *testing.T) {
	tests := []struct {
		name         string
		maxOpenFiles int
		expected     int
	}{
		{
			name:         "normal case",
			maxOpenFiles: 200,
			expected:     190, // 200 - 10 reserved
		},
		{
			name:         "small maxOpenFiles uses minimum",
			maxOpenFiles: 50,
			expected:     64, // Uses MinFileCacheSize
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := DefaultOptions()
			opts.MaxOpenFiles = tt.maxOpenFiles
			result := opts.GetFileCacheSize()
			if result != tt.expected {
				t.Errorf("GetFileCacheSize() = %d, expected %d", result, tt.expected)
			}
		})
	}
}

func TestOptionsValidation(t *testing.T) {
	tests := []struct {
		name        string
		setupOpts   func(*Options)
		expectError bool
		errorType   error
	}{
		{
			name: "valid options",
			setupOpts: func(o *Options) {
				// Use defaults
			},
			expectError: false,
		},
		{
			name: "invalid MaxOpenFiles",
			setupOpts: func(o *Options) {
				o.MaxOpenFiles = 0
			},
			expectError: true,
			errorType:   ErrInvalidMaxOpenFiles,
		},
		{
			name: "small MaxOpenFiles still works",
			setupOpts: func(o *Options) {
				o.MaxOpenFiles = 50 // Will use minimum cache size
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := DefaultOptions()
			opts.Path = filepath.Join(os.TempDir(), "test") // Set a valid path for validation
			tt.setupOpts(opts)

			err := opts.Validate()
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				} else if tt.errorType != nil && err != tt.errorType {
					t.Errorf("Expected error %v, got %v", tt.errorType, err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}
		})
	}
}
