#!/bin/bash

# capture-benchmarks.sh - Capture Go benchmark results with metadata
# Usage: ./scripts/capture-benchmarks.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/benchmarks/results"

# Ensure results directory exists
mkdir -p "$RESULTS_DIR"

# Generate timestamp
TIMESTAMP=$(date +"%Y-%m-%d-%H%M%S")
RESULT_FILE="$RESULTS_DIR/$TIMESTAMP.json"

echo "Capturing benchmarks at $(date)..."
echo "Output: $RESULT_FILE"

# Get system and git information
GO_VERSION=$(go version | cut -d' ' -f3)
COMMIT=$(cd "$PROJECT_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BRANCH=$(cd "$PROJECT_ROOT" && git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
UNAME=$(uname -m)
OS=$(uname -s)

# Run benchmarks with JSON output using tools.go or direct parsing
cd "$PROJECT_ROOT"
BENCH_OUTPUT=$(go test -bench=. -benchmem -timeout=30m ./... -json 2>&1 | tail -1000)

# Create JSON result file
cat > "$RESULT_FILE" << EOF
{
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "unix_timestamp": $(date +%s),
  "go_version": "$GO_VERSION",
  "commit": "$COMMIT",
  "branch": "$BRANCH",
  "os": "$OS",
  "arch": "$UNAME",
  "benchmarks": {}
}
EOF

# Parse benchmark output and update JSON
# Format: BenchmarkName-4    	       N	  ns/op	  B/op	  allocs/op
echo "$BENCH_OUTPUT" | grep "^Benchmark" | while read line; do
    # Extract benchmark name and metrics
    name=$(echo "$line" | awk '{print $1}')
    ops=$(echo "$line" | awk '{print $3}' | sed 's/ns\/op//')
    bytes=$(echo "$line" | awk '{print $4}' | sed 's/B\/op//')
    allocs=$(echo "$line" | awk '{print $5}' | sed 's/allocs\/op//')

    # Add to JSON using jq if available, otherwise use sed/awk
    if command -v jq &> /dev/null; then
        jq ".benchmarks[\"$name\"] = {
          \"ns_per_op\": $ops,
          \"bytes_per_op\": $bytes,
          \"allocs_per_op\": $allocs
        }" "$RESULT_FILE" > "$RESULT_FILE.tmp" && mv "$RESULT_FILE.tmp" "$RESULT_FILE"
    fi
done

# If jq is not available, create a simpler format
if ! command -v jq &> /dev/null; then
    # Remove the empty benchmarks object and use a simpler format
    cat > "$RESULT_FILE" << EOF
{
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "unix_timestamp": $(date +%s),
  "go_version": "$GO_VERSION",
  "commit": "$COMMIT",
  "branch": "$BRANCH",
  "os": "$OS",
  "arch": "$UNAME",
  "raw_output": "$(echo "$BENCH_OUTPUT" | sed 's/"/\\"/g')"
}
EOF
fi

echo "✓ Benchmark results saved to: $RESULT_FILE"
echo ""
echo "To compare with previous results:"
echo "  ./scripts/compare-benchmarks.sh $RESULT_FILE <previous_result_file>"
