#!/bin/bash

# diff-benchmarks.sh - Simple diff between two benchmark runs
# Usage: ./scripts/diff-benchmarks.sh <file1> <file2>
#   or:  ./scripts/diff-benchmarks.sh  (compares latest two)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/results"

if [ $# -eq 2 ]; then
    FILE1="$1"
    FILE2="$2"
elif [ $# -eq 0 ]; then
    # Get the two most recent files
    FILE1=$(ls -t "$RESULTS_DIR"/*.txt 2>/dev/null | sed -n '2p')
    FILE2=$(ls -t "$RESULTS_DIR"/*.txt 2>/dev/null | sed -n '1p')

    if [ -z "$FILE1" ] || [ -z "$FILE2" ]; then
        echo "Error: Need at least 2 benchmark result files"
        echo "Run: ./scripts/save-benchmarks.sh"
        exit 1
    fi
else
    echo "Usage: $0 [file1] [file2]"
    echo "   or: $0  (auto-compare latest two)"
    exit 1
fi

echo "Comparing: $(basename $FILE1) vs $(basename $FILE2)"
echo ""

# Extract benchmark lines from both files
BENCH1=$(grep "^Benchmark" "$FILE1")
BENCH2=$(grep "^Benchmark" "$FILE2")

# Show metadata
echo "=== Before ==="
grep "^# " "$FILE1" | head -5
echo ""
echo "=== After ==="
grep "^# " "$FILE2" | head -5
echo ""
echo "=== Comparison ==="
echo ""

# Create a comparison table
printf "%-45s %12s %12s %10s\n" "Benchmark" "Before" "After" "Change"
echo "==============================================================================="

# Process each benchmark
echo "$BENCH1" | while read line1; do
    name=$(echo "$line1" | awk '{print $1}')
    before=$(echo "$line1" | awk '{print $3}')

    # Find the same benchmark in file2
    line2=$(echo "$BENCH2" | grep "^$name " | head -1)
    if [ -z "$line2" ]; then
        continue
    fi

    after=$(echo "$line2" | awk '{print $3}')

    # Extract just the numeric part (remove 'ns/op', etc)
    before_num=$(echo "$before" | sed 's/[^0-9.]//g')
    after_num=$(echo "$after" | sed 's/[^0-9.]//g')

    if [ -z "$before_num" ] || [ -z "$after_num" ]; then
        continue
    fi

    # Calculate percentage change
    if [ "$(echo "$before_num != 0" | bc 2>/dev/null || echo 1)" -eq 1 ]; then
        change=$(echo "scale=2; (($after_num - $before_num) / $before_num) * 100" | bc 2>/dev/null || echo "?")
    else
        change="?"
    fi

    # Format output
    printf "%-45s %12s %12s" "$name" "$before" "$after"

    if [ "$change" != "?" ]; then
        if (( $(echo "$change > 5" | bc -l 2>/dev/null || echo 0) )); then
            printf " ↑ %+.1f%%\n" "$change"
        elif (( $(echo "$change < -5" | bc -l 2>/dev/null || echo 0) )); then
            printf " ↓ %+.1f%%\n" "$change"
        else
            printf " %+.1f%%\n" "$change"
        fi
    else
        printf " %s\n" "$change"
    fi
done

echo ""
echo "Legend: ↑ = regression (>5% slower), ↓ = improvement (>5% faster)"
