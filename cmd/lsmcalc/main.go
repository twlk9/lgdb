// lsmcalc is a calculator for LSM tree file/size/layout estimation.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	KiB = 1024
	MiB = KiB * 1024
	GiB = MiB * 1024
	TiB = GiB * 1024
)

type Config struct {
	TotalSize             int64   // Target total database size
	WriteBufferSize       int64   // Base memtable/L0 file size
	LevelSizeMultiplier   float64 // Size multiplier between levels
	LevelFileSizeMultiplier float64 // File size multiplier between levels
	MaxLevels             int     // Maximum number of levels (including L0)
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	cfg := Config{
		TotalSize:             500 * GiB,
		WriteBufferSize:       4 * MiB,
		LevelSizeMultiplier:   10.0,
		LevelFileSizeMultiplier: 2.0,
		MaxLevels:             7,
	}

	fmt.Println("LSM Tree Layout Calculator")
	fmt.Println("==========================")
	fmt.Println("Press Enter to accept defaults shown in brackets.\n")

	cfg.TotalSize = promptSize(reader, "Total database size", cfg.TotalSize)
	cfg.WriteBufferSize = promptSize(reader, "Write buffer size (L0 file size)", cfg.WriteBufferSize)
	cfg.LevelSizeMultiplier = promptFloat(reader, "Level size multiplier", cfg.LevelSizeMultiplier)
	cfg.LevelFileSizeMultiplier = promptFloat(reader, "Level file size multiplier", cfg.LevelFileSizeMultiplier)
	cfg.MaxLevels = promptInt(reader, "Max levels (including L0)", cfg.MaxLevels)

	fmt.Println()
	printLayout(cfg)
}

func promptSize(reader *bufio.Reader, prompt string, defaultVal int64) int64 {
	fmt.Printf("%s [%s]: ", prompt, formatSize(defaultVal))
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input == "" {
		return defaultVal
	}
	return parseSize(input, defaultVal)
}

func promptFloat(reader *bufio.Reader, prompt string, defaultVal float64) float64 {
	fmt.Printf("%s [%.1f]: ", prompt, defaultVal)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input == "" {
		return defaultVal
	}
	val, err := strconv.ParseFloat(input, 64)
	if err != nil {
		return defaultVal
	}
	return val
}

func promptInt(reader *bufio.Reader, prompt string, defaultVal int) int {
	fmt.Printf("%s [%d]: ", prompt, defaultVal)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	if input == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(input)
	if err != nil {
		return defaultVal
	}
	return val
}

func parseSize(s string, defaultVal int64) int64 {
	s = strings.ToUpper(strings.TrimSpace(s))
	multiplier := int64(1)

	if strings.HasSuffix(s, "TB") || strings.HasSuffix(s, "T") {
		multiplier = TiB
		s = strings.TrimSuffix(strings.TrimSuffix(s, "TB"), "T")
	} else if strings.HasSuffix(s, "GB") || strings.HasSuffix(s, "G") {
		multiplier = GiB
		s = strings.TrimSuffix(strings.TrimSuffix(s, "GB"), "G")
	} else if strings.HasSuffix(s, "MB") || strings.HasSuffix(s, "M") {
		multiplier = MiB
		s = strings.TrimSuffix(strings.TrimSuffix(s, "MB"), "M")
	} else if strings.HasSuffix(s, "KB") || strings.HasSuffix(s, "K") {
		multiplier = KiB
		s = strings.TrimSuffix(strings.TrimSuffix(s, "KB"), "K")
	}

	val, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return defaultVal
	}
	return int64(val * float64(multiplier))
}

func formatSize(bytes int64) string {
	switch {
	case bytes >= TiB:
		return fmt.Sprintf("%.1fTB", float64(bytes)/float64(TiB))
	case bytes >= GiB:
		return fmt.Sprintf("%.1fGB", float64(bytes)/float64(GiB))
	case bytes >= MiB:
		return fmt.Sprintf("%.1fMB", float64(bytes)/float64(MiB))
	case bytes >= KiB:
		return fmt.Sprintf("%.1fKB", float64(bytes)/float64(KiB))
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}

func printLayout(cfg Config) {
	fmt.Println("LSM Tree Layout")
	fmt.Println("===============")
	fmt.Printf("Target Size: %s\n\n", formatSize(cfg.TotalSize))

	// Header
	fmt.Printf("%-8s  %15s  %15s  %12s\n", "Level", "File Size", "Level Size", "File Count")
	fmt.Printf("%-8s  %15s  %15s  %12s\n", "-----", "---------", "----------", "----------")

	var totalSize int64
	var totalFiles int64

	// L1 base size = WriteBufferSize * LevelFileSizeMultiplier * 10
	// (following the pattern from options.go: baseSize = TargetFileSize(1) * 10)
	l1FileSize := float64(cfg.WriteBufferSize) * cfg.LevelFileSizeMultiplier
	l1LevelSize := l1FileSize * 10

	for level := 0; level < cfg.MaxLevels; level++ {
		var fileSize int64
		var levelSize int64
		var fileCount int64

		if level == 0 {
			// L0: files are memtable size, count is based on compaction trigger (typically 4-12)
			fileSize = cfg.WriteBufferSize
			// Show a representative L0 size (4 files typical before compaction)
			fileCount = 4
			levelSize = fileSize * fileCount
		} else if level == 1 {
			fileSize = int64(l1FileSize)
			levelSize = int64(l1LevelSize)
			fileCount = levelSize / fileSize
		} else {
			// Each level: file size grows by LevelFileSizeMultiplier
			// Level size grows by LevelSizeMultiplier
			fileMult := 1.0
			levelMult := 1.0
			for i := 2; i <= level; i++ {
				fileMult *= cfg.LevelFileSizeMultiplier
				levelMult *= cfg.LevelSizeMultiplier
			}
			fileSize = int64(l1FileSize * fileMult)
			levelSize = int64(l1LevelSize * levelMult)
			fileCount = levelSize / fileSize
			if fileCount < 1 {
				fileCount = 1
			}
		}

		fmt.Printf("L%-7d  %15s  %15s  %12d\n", level, formatSize(fileSize), formatSize(levelSize), fileCount)
		totalSize += levelSize
		totalFiles += fileCount

		// Stop if we've exceeded target size
		if totalSize >= cfg.TotalSize {
			break
		}
	}

	fmt.Printf("%-8s  %15s  %15s  %12s\n", "-----", "---------", "----------", "----------")
	fmt.Printf("%-8s  %15s  %15s  %12d\n", "TOTAL", "", formatSize(totalSize), totalFiles)

	if totalSize < cfg.TotalSize {
		fmt.Printf("\nNote: Max levels reached. Actual capacity: %s (%.1f%% of target)\n",
			formatSize(totalSize), float64(totalSize)/float64(cfg.TotalSize)*100)
	}
}
