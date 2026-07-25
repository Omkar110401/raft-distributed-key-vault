#!/usr/bin/env bash
set -euo pipefail

# Sweep runner: runs multiple benchmark configs, repeats, and aggregates CSVs
TS=$(date -u +%Y%m%dT%H%M%SZ)
OUTDIR=benchmarks/results/$TS
mkdir -p "$OUTDIR"

echo "Starting cluster (build if needed)"
docker-compose up -d --build

echo "Waiting for nodes to become ready..."
sleep 12

# parameter grid (edit as needed)
PATTERNS=("CONSTANT" "BURST" "RAMP_UP")
REQUESTS=(100 500)
DURATIONS=(10 30)
REPEATS=3

HEADER_WRITTEN=0

for pattern in "${PATTERNS[@]}"; do
  for req in "${REQUESTS[@]}"; do
    for dur in "${DURATIONS[@]}"; do
      for rep in $(seq 1 $REPEATS); do
        cfg=$(cat <<EOF
{"pattern":"$pattern","totalRequests":$req,"durationSeconds":$dur,"burstSize":10,"rampSteps":5}
EOF
)
        name="${pattern}_req${req}_dur${dur}_rep${rep}"
        jsonf="$OUTDIR/${name}.json"
        csvf="$OUTDIR/${name}.csv"

        echo "Running $name"
        curl -s -X POST http://localhost:8080/api/benchmark/run \
          -H "Content-Type: application/json" \
          -d "$cfg" -o "$jsonf"

        echo "Exporting CSV for $name"
        curl -s -X POST http://localhost:8080/api/benchmark/export \
          -H "Content-Type: application/json" \
          -d @"$jsonf" -o "$csvf"

        # Export per-request latencies if available
        latcsv="$OUTDIR/${name}_latencies.csv"
        if [ -f "$jsonf" ]; then
          jq -r '.[] | .latencies | to_entries[] | [.key, .value] | @csv' "$jsonf" > "$latcsv" || true
        fi

        # aggregate CSVs (preserve header once)
        if [ "$HEADER_WRITTEN" -eq 0 ]; then
          cat "$csvf" > "$OUTDIR/aggregate.csv"
          HEADER_WRITTEN=1
        else
          tail -n +2 "$csvf" >> "$OUTDIR/aggregate.csv"
        fi

      done
    done
  done
done

echo "Aggregated CSV: $OUTDIR/aggregate.csv"

echo "Shutting down cluster"
docker-compose down

echo "Done"
