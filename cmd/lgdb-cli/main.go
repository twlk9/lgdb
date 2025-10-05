package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/twlk9/lgdb"
	"github.com/twlk9/lgdb/compression"
	"github.com/twlk9/lgdb/keys"
	"github.com/twlk9/lgdb/sstable"
)

const version = "1.0.0"

func main() {
	flag.Usage = printUsage

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	args := os.Args[2:]

	switch command {
	case "list":
		if err := listCommand(args); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "dump":
		if err := dumpCommand(args); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "compact":
		if err := compactCommand(args); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "verify":
		if err := verifyCommand(args); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "scan-key":
		if err := scanKeyCommand(args); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "version":
		fmt.Printf("lgdb-cli version %s\n", version)
	case "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Printf(`lgdb-cli - Command line tool for inspecting LevelGoDB databases

Usage:
  lgdb-cli <command> [options]

Commands:
  list <db_path>                     List all SSTables with sizes, levels, and key ranges
  dump <db_path> <file_number>       Dump contents of specific SSTable file
  compact <db_path>                  Force database compaction
  verify <db_path>                   Verify database integrity
  scan-key <db_path> <key_prefix>    Find which SSTables contain keys with given prefix
  version                            Show version information
  help                               Show this help message

Examples:
  lgdb-cli list /path/to/database
  lgdb-cli dump /path/to/database 000001
  lgdb-cli compact /path/to/database
  lgdb-cli verify /path/to/database
  lgdb-cli scan-key /path/to/database "fr\\x00nodeid"

`)
}

func listCommand(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("list command requires database path")
	}

	dbPath := args[0]

	// Check if database directory exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database directory does not exist: %s", dbPath)
	}

	// Open database in read-only mode
	opts := lgdb.DefaultOptions()
	opts.Path = dbPath
	opts.ReadOnly = false // Need write access to read latest manifest
	db, err := lgdb.Open(opts)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	// Get database statistics to access SSTable information
	stats := db.GetStats()
	if stats == nil {
		return fmt.Errorf("could not access database stats")
	}

	// Extract level information from stats
	levelStats := stats["levels"].(map[string]int)
	levelSizes := stats["level_sizes"].(map[string]int64)

	// Calculate total files
	totalFiles := 0
	for level := range 7 {
		key := fmt.Sprintf("level_%d_files", level)
		if count, ok := levelStats[key]; ok {
			totalFiles += count
		}
	}

	if totalFiles == 0 {
		fmt.Println("No SSTables found in database")
		return nil
	}

	fmt.Printf("Database: %s\n", dbPath)
	fmt.Printf("Total SSTables: %d\n\n", totalFiles)

	// Print files organized by level
	for level := range 7 {
		fileCountKey := fmt.Sprintf("level_%d_files", level)
		fileSizeKey := fmt.Sprintf("level_%d_size", level)

		fileCount, hasFiles := levelStats[fileCountKey]
		fileSize, _ := levelSizes[fileSizeKey]

		if !hasFiles || fileCount == 0 {
			continue
		}

		fmt.Printf("Level %d (%d files):\n", level, fileCount)
		fmt.Printf("  Total size: %s\n", formatBytes(uint64(fileSize)))
		fmt.Printf("  Average file size: %s\n", formatBytes(uint64(fileSize)/uint64(fileCount)))
		fmt.Printf("  Estimated keys: ~%d\n\n", estimateKeyCount(uint64(fileSize)))
	}

	return nil
}

func dumpCommand(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("dump command requires database path and file number")
	}

	dbPath := args[0]
	fileNumStr := args[1]

	fileNum, err := strconv.ParseUint(fileNumStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid file number: %s", fileNumStr)
	}

	// Construct SSTable file path
	sstablePath := filepath.Join(dbPath, fmt.Sprintf("%06d.sst", fileNum))

	// Check if file exists
	if _, err := os.Stat(sstablePath); os.IsNotExist(err) {
		return fmt.Errorf("SSTable file does not exist: %s", sstablePath)
	}

	// Open SSTable reader
	reader, err := sstable.NewSSTableReader(sstablePath, fileNum, nil, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	if err != nil {
		return fmt.Errorf("failed to open SSTable: %v", err)
	}
	defer reader.Close()

	fmt.Printf("SSTable: %s\n", sstablePath)

	// Get file size
	fileInfo, err := os.Stat(sstablePath)
	if err == nil {
		fmt.Printf("File size: %s\n", formatBytes(uint64(fileInfo.Size())))
	}

	fmt.Printf("Contents:\n\n")

	// Create iterator and dump all entries
	iter := reader.NewIterator()
	defer iter.Close()

	count := 0
	iter.SeekToFirst()

	fmt.Printf("%-6s %-30s %-30s %-10s %s\n", "Index", "Key (User)", "Value", "Type", "Size")
	fmt.Printf("%s\n", "---------------------------------------------------------------------------------")

	for iter.Valid() {
		count++
		internalKey := iter.Key()
		value := iter.Value()

		// Format the user key and value
		keyStr := formatKey(internalKey.UserKey(), 28)
		valueStr := formatValue(value, 28)

		// Determine entry type from internal key kind
		entryType := "DATA"
		switch internalKey.Kind() {
		case keys.KindSet:
			entryType = "SET"
		case keys.KindDelete:
			entryType = "DELETE"
		case keys.KindRangeDelete:
			entryType = "RANGE_DEL"
		default:
			entryType = "UNKNOWN"
		}

		sizeStr := formatBytes(uint64(len(internalKey) + len(value)))
		fmt.Printf("%-6d %-30s %-30s %-10s %s\n", count, keyStr, valueStr, entryType, sizeStr)

		iter.Next()

		// Limit output for very large files
		if count >= 1000 {
			fmt.Printf("... (showing first 1000 entries, file may contain more)\n")
			break
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %v", err)
	}

	fmt.Printf("\nTotal entries shown: %d\n", count)
	return nil
}

func compactCommand(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("compact command requires database path")
	}

	dbPath := args[0]

	// Check if database directory exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database directory does not exist: %s", dbPath)
	}

	// Open database
	opts := lgdb.DefaultOptions()
	opts.Path = dbPath
	opts.Compression = compression.ZstdBalancedConfig()
	// opts.Compression = compression.SnappyConfig()
	opts.BlockSize = 32 * lgdb.KiB
	opts.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	fmt.Printf("L0CompactionTrigger: %d\n", opts.L0CompactionTrigger)
	db, err := lgdb.Open(opts)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	fmt.Printf("Starting manual compaction of database: %s\n", dbPath)

	// Get stats before
	statsBefore := getCompactionStats(db)
	fmt.Printf("Before: L0=%d, L1=%d, L2=%d, Total=%d\n",
		statsBefore["level_0_files"], statsBefore["level_1_files"],
		statsBefore["level_2_files"], statsBefore["total_files"])

	// Use the new CompactAll method that handles the looping internally
	if err := db.CompactAll(); err != nil {
		return fmt.Errorf("compaction failed: %v", err)
	}

	// Get stats after
	statsAfter := getCompactionStats(db)
	fmt.Printf("After:  L0=%d, L1=%d, L2=%d, Total=%d\n",
		statsAfter["level_0_files"], statsAfter["level_1_files"],
		statsAfter["level_2_files"], statsAfter["total_files"])

	// Note: Orphaned files will be cleaned up automatically on next database startup

	fmt.Println("Compaction completed successfully")
	return nil
}

// getCompactionStats extracts relevant compaction statistics from the database
func getCompactionStats(db *lgdb.DB) map[string]int {
	stats := db.GetStats()
	if stats == nil {
		return make(map[string]int)
	}

	levelStats, ok := stats["levels"].(map[string]int)
	if !ok {
		return make(map[string]int)
	}

	result := make(map[string]int)
	totalFiles := 0

	// Extract file counts for each level
	for level := range 7 {
		key := fmt.Sprintf("level_%d_files", level)
		if count, exists := levelStats[key]; exists {
			result[key] = count
			totalFiles += count
		} else {
			result[key] = 0
		}
	}
	result["total_files"] = totalFiles

	return result
}

// statsEqual checks if two stat maps have the same file counts
func statsEqual(before, after map[string]int) bool {
	if len(before) != len(after) {
		return false
	}

	for key, beforeValue := range before {
		if afterValue, exists := after[key]; !exists || beforeValue != afterValue {
			return false
		}
	}

	return true
}

func verifyCommand(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("verify command requires database path")
	}

	dbPath := args[0]

	// Check if database directory exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database directory does not exist: %s", dbPath)
	}

	// Open database in read-only mode
	opts := lgdb.DefaultOptions()
	opts.Path = dbPath
	opts.ReadOnly = false // Need write access to read latest manifest
	db, err := lgdb.Open(opts)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	fmt.Printf("Verifying database integrity: %s\n", dbPath)

	// Test basic iteration to verify data can be read
	iter := db.NewIterator()
	defer iter.Close()

	count := 0
	sampleCount := 0
	iter.SeekToFirst()

	fmt.Println("Reading data...")
	for iter.Valid() {
		key := iter.Key()
		value := iter.Value()

		// Show first few entries as samples
		if sampleCount < 5 {
			fmt.Printf("  Sample %d: Key=%s, Value=%s\n", sampleCount+1,
				formatKey(key, 20), formatValue(value, 20))
			sampleCount++
		}

		count++
		iter.Next()

		// Don't iterate through massive databases
		if count >= 10000 {
			fmt.Println("  ... (limiting verification to first 10,000 entries)")
			break
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error during verification: %v", err)
	}

	fmt.Printf("✓ Successfully verified %d entries\n", count)

	// Get and display database stats
	stats := db.GetStats()
	if levelStats, ok := stats["levels"].(map[string]int); ok {
		totalFiles := 0
		for level := range 7 {
			key := fmt.Sprintf("level_%d_files", level)
			if fileCount, exists := levelStats[key]; exists {
				totalFiles += fileCount
			}
		}
		fmt.Printf("✓ Database has %d SSTable files across all levels\n", totalFiles)
	}

	fmt.Println("✓ Database integrity verified successfully!")
	return nil
}

func scanKeyCommand(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("scan-key command requires database path and key prefix")
	}

	dbPath := args[0]
	keyPrefix := args[1]

	// Check if database directory exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database directory does not exist: %s", dbPath)
	}

	// Convert hex sequences in key prefix (e.g., "\\x00" -> actual null byte)
	decodedPrefix := make([]byte, 0, len(keyPrefix))
	for i := 0; i < len(keyPrefix); i++ {
		if i+4 <= len(keyPrefix) && keyPrefix[i:i+2] == "\\x" {
			// Parse hex byte
			hexStr := keyPrefix[i+2 : i+4]
			b := byte(0)
			for _, c := range hexStr {
				b *= 16
				if c >= '0' && c <= '9' {
					b += byte(c - '0')
				} else if c >= 'a' && c <= 'f' {
					b += byte(c - 'a' + 10)
				} else if c >= 'A' && c <= 'F' {
					b += byte(c - 'A' + 10)
				}
			}
			decodedPrefix = append(decodedPrefix, b)
			i += 3 // Skip the hex sequence
		} else {
			decodedPrefix = append(decodedPrefix, keyPrefix[i])
		}
	}

	fmt.Printf("Scanning for keys with prefix: %s\n", formatKey(decodedPrefix, 50))
	fmt.Printf("Database: %s\n\n", dbPath)

	// Open database in read-only mode
	opts := lgdb.DefaultOptions()
	opts.Path = dbPath
	opts.ReadOnly = false // Need write access to read latest manifest
	db, err := lgdb.Open(opts)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	// Get all SSTable files from the database
	sstFiles, err := filepath.Glob(filepath.Join(dbPath, "*.sst"))
	if err != nil {
		return fmt.Errorf("failed to list SST files: %v", err)
	}

	if len(sstFiles) == 0 {
		fmt.Println("No SSTable files found")
		return nil
	}

	foundFiles := 0
	totalKeys := 0

	// Check each SSTable file
	for _, sstFile := range sstFiles {
		// Extract file number
		filename := filepath.Base(sstFile)
		if filepath.Ext(filename) != ".sst" {
			continue
		}
		fileNumStr := filename[:len(filename)-4] // Remove .sst extension

		fileNum, err := strconv.ParseUint(fileNumStr, 10, 64)
		if err != nil {
			continue
		}

		// Open SSTable reader
		reader, err := sstable.NewSSTableReader(sstFile, fileNum, nil, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
		if err != nil {
			fmt.Printf("Failed to open SSTable %s: %v\n", sstFile, err)
			continue
		}

		// Create iterator and check for matching keys
		iter := reader.NewIterator()
		iter.SeekToFirst()

		matchingKeys := 0
		samples := make([]string, 0, 5)

		for iter.Valid() {
			key := iter.Key()
			userKey := key.UserKey()

			// Check if this key matches our prefix
			if len(userKey) >= len(decodedPrefix) && string(userKey[:len(decodedPrefix)]) == string(decodedPrefix) {
				matchingKeys++
				totalKeys++

				// Collect samples (first 5 matching keys)
				if len(samples) < 5 {
					samples = append(samples, formatKey(userKey, 60))
				}
			}

			iter.Next()
		}

		iter.Close()
		reader.Close()

		// Report if this file has matching keys
		if matchingKeys > 0 {
			foundFiles++
			fmt.Printf("File: %06d.sst (%d matching keys)\n", fileNum, matchingKeys)

			// Show sample keys
			for i, sample := range samples {
				fmt.Printf("  Sample %d: %s\n", i+1, sample)
			}
			if matchingKeys > len(samples) {
				fmt.Printf("  ... (%d more keys)\n", matchingKeys-len(samples))
			}
			fmt.Println()
		}
	}

	// Summary
	if foundFiles == 0 {
		fmt.Printf("No keys found with prefix: %s\n", formatKey(decodedPrefix, 50))
	} else {
		fmt.Printf("Summary: Found %d keys across %d SSTable files\n", totalKeys, foundFiles)
	}

	return nil
}

// Helper functions for formatting output

func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatKey(key []byte, maxLen int) string {
	if len(key) == 0 {
		return "<empty>"
	}

	// Convert to string, showing printable chars and hex for non-printable
	str := ""
	for _, b := range key {
		if b >= 32 && b <= 126 {
			str += string(b)
		} else {
			str += fmt.Sprintf("\\x%02x", b)
		}
	}

	if len(str) > maxLen {
		return str[:maxLen-3] + "..."
	}
	return str
}

func formatValue(value []byte, maxLen int) string {
	if len(value) == 0 {
		return "<empty>"
	}

	// Values are stored directly without encoding in the current implementation
	return formatKey(value, maxLen)
}

func estimateKeyCount(fileSize uint64) uint64 {
	// Very rough estimate: assume average key+value size of 50 bytes
	// This is just for display purposes
	avgEntrySize := uint64(50)
	return fileSize / avgEntrySize
}
