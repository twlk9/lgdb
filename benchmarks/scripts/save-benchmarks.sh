#!/bin/bash

# save-benchmarks.sh - Simple script to capture and save benchmark results
# Usage: ./scripts/save-benchmarks.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/benchmarks/results"

mkdir -p "$RESULTS_DIR"

# Generate filename with timestamp and git info
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
COMMIT=$(cd "$PROJECT_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BRANCH=$(cd "$PROJECT_ROOT" && git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
FILENAME="$RESULTS_DIR/${TIMESTAMP}_${COMMIT}.txt"

echo "Running benchmarks..."
echo "Saving to: $FILENAME"
echo ""

cd "$PROJECT_ROOT"

# Run benchmarks and capture output with metadata
{
    echo "# Benchmark Results"
    echo "# Timestamp: $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
    echo "# Commit: $COMMIT"
    echo "# Branch: $BRANCH"
    echo "# Go version: $(go version)"
    echo "# System: $(uname -s) $(uname -m)"
    echo ""
    echo "## Raw Benchmark Output:"
    echo ""
    go test -bench=. -benchmem -timeout=30m ./... 2>&1 | grep -E "^(Benchmark|ok|FAIL)" || true
} > "$FILENAME"

echo "✓ Results saved: $FILENAME"
echo ""
echo "Sample results from this run:"
grep "^Benchmark" "$FILENAME" | head -10
