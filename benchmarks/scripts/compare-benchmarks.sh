#!/bin/bash

# compare-benchmarks.sh - Compare two benchmark runs
# Usage:
#   ./scripts/compare-benchmarks.sh <file1> <file2>
#   ./scripts/compare-benchmarks.sh                    # Compare latest two results

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/benchmarks/results"

# Helper function to extract benchmark metrics from raw output
parse_benchmarks() {
    local file=$1
    # Extract the raw benchmark lines and save to temp file
    if grep -q "raw_output" "$file" 2>/dev/null; then
        # If stored as raw_output, extract it
        grep "raw_output" "$file" | sed 's/.*"raw_output": "//' | sed 's/"$//' | sed 's/\\n/\n/g'
    else
        # Otherwise assume it's structured JSON
        cat "$file"
    fi
}

# Extract benchmark name and ns/op value
extract_metric() {
    local line=$1
    local metric=$2  # "ns/op", "B/op", or "allocs/op"

    # Extract the value before the metric label
    echo "$line" | grep -o "[0-9]*[.]*[0-9]*[^ ]* $metric" | sed "s/ $metric//" || echo "0"
}

# Format numbers with thousands separator
format_number() {
    printf "%'d" "$1" 2>/dev/null || echo "$1"
}

# Calculate percentage change
calc_percent() {
    local old=$1
    local new=$2
    if [ "$(echo "$old == 0" | bc)" -eq 1 ]; then
        echo "N/A"
        return
    fi
    local diff=$(echo "scale=1; (($new - $old) / $old) * 100" | bc 2>/dev/null || echo "0")
    if [ "$diff" != "0" ]; then
        echo "$diff%"
    else
        echo "0%"
    fi
}

# Color codes (for terminals that support it)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Determine which files to compare
if [ $# -eq 2 ]; then
    FILE1="$1"
    FILE2="$2"
elif [ $# -eq 0 ]; then
    # Find the two most recent result files
    FILES=($(ls -t "$RESULTS_DIR"/*.json 2>/dev/null | head -2))
    if [ ${#FILES[@]} -lt 2 ]; then
        echo "Error: Need at least 2 benchmark result files to compare"
        echo "Found: ${#FILES[@]} file(s)"
        echo ""
        echo "Capture benchmarks first:"
        echo "  ./scripts/capture-benchmarks.sh"
        exit 1
    fi
    FILE2="${FILES[0]}"  # Most recent
    FILE1="${FILES[1]}"  # Second most recent
else
    echo "Usage: $0 [file1] [file2]"
    exit 1
fi

if [ ! -f "$FILE1" ]; then
    echo "Error: File not found: $FILE1"
    exit 1
fi

if [ ! -f "$FILE2" ]; then
    echo "Error: File not found: $FILE2"
    exit 1
fi

echo "Comparing benchmark results..."
echo ""
echo "Before: $(basename $FILE1)"
echo "After:  $(basename $FILE2)"
echo ""

# Extract metadata
COMMIT1=$(grep '"commit"' "$FILE1" | head -1 | cut -d'"' -f4)
COMMIT2=$(grep '"commit"' "$FILE2" | head -1 | cut -d'"' -f4)
TIME1=$(grep '"timestamp"' "$FILE1" | head -1 | cut -d'"' -f4)
TIME2=$(grep '"timestamp"' "$FILE2" | head -1 | cut -d'"' -f4)

echo "Commit: $COMMIT1 → $COMMIT2"
echo "Time:   $TIME1 → $TIME2"
echo ""

# Parse the raw benchmark output from both files
BENCH1=$(parse_benchmarks "$FILE1")
BENCH2=$(parse_benchmarks "$FILE2")

# Create comparison header
printf "%-40s %15s %15s %10s\n" "Benchmark" "Before" "After" "Change"
echo "------------------------------------------------------------------------"

# Extract benchmark names from the first file and compare
echo "$BENCH1" | grep "^Benchmark" | while read line1; do
    bench_name=$(echo "$line1" | awk '{print $1}')

    # Find matching benchmark in second file
    line2=$(echo "$BENCH2" | grep "^$bench_name" | head -1)

    if [ -z "$line2" ]; then
        continue
    fi

    # Extract ns/op values
    ns_op_1=$(extract_metric "$line1" "ns/op")
    ns_op_2=$(extract_metric "$line2" "ns/op")

    # Strip trailing characters if present
    ns_op_1=$(echo "$ns_op_1" | sed 's/[^0-9.]//g')
    ns_op_2=$(echo "$ns_op_2" | sed 's/[^0-9.]//g')

    if [ -z "$ns_op_1" ] || [ -z "$ns_op_2" ]; then
        continue
    fi

    # Calculate change
    change=$(echo "scale=1; (($ns_op_2 - $ns_op_1) / $ns_op_1) * 100" | bc 2>/dev/null || echo "0")

    # Format the output with color coding
    printf "%-40s" "$bench_name"
    printf " %15s" "$(format_number ${ns_op_1%.*})"
    printf " %15s" "$(format_number ${ns_op_2%.*})"

    # Color code the change
    if [ "$(echo "$change > 5" | bc 2>/dev/null || echo 0)" -eq 1 ]; then
        printf " ${RED}%+.1f%%${NC}\n" "$change"
    elif [ "$(echo "$change < -5" | bc 2>/dev/null || echo 0)" -eq 1 ]; then
        printf " ${GREEN}%+.1f%%${NC}\n" "$change"
    else
        printf " %+.1f%%\n" "$change"
    fi
done

echo ""
echo "Legend: + = slower, - = faster"
