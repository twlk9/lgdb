# Benchmark Results

This directory stores benchmark results over time to track performance
changes.

## Structure

- `results/` - Contains timestamped benchmark results (JSON format)
- `scripts/` - Helper scripts for capturing and comparing benchmarks

## Capturing Benchmarks

Run benchmarks and save results:

```bash
./scripts/capture-benchmarks.sh
```

This creates a new file like `results/2025-10-20-143022.json` with all
benchmark data.

## Comparing Results

Compare two benchmark runs:

```bash
./scripts/compare-benchmarks.sh results/2025-10-20-143022.json results/2025-10-20-150000.json
```

Or compare latest with previous:

```bash
./scripts/compare-benchmarks.sh
```

## Interpreting Results

The comparison shows:
- **+50%** = 50% slower (ns/op increased)
- **-30%** = 30% faster (ns/op decreased)
- Values highlighted in red indicate significant regressions (>5%
  slower)
- Values highlighted in green indicate improvements (>5% faster)

## Notes

- Results are stored in JSON for easy parsing and comparison
- Each result includes the Go version, CPU info, and timestamp
- Git commit hash is captured for correlation with code changes
