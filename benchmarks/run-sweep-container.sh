#!/usr/bin/env bash
set -euo pipefail

# Container-friendly sweep runner. Assumes cluster services are available at node1:8080, node2:8081, node3:8082
TS=$(date -u +%Y%m%dT%H%M%SZ)
OUTDIR=/bench/results/$TS
mkdir -p "$OUTDIR"

echo "Waiting for node1 to be ready..."
for i in $(seq 1 30); do
  if curl -sSf http://node1:8080/actuator/health >/dev/null 2>&1; then
    echo "node1 ready"
    break
  fi
  sleep 1
done

PATTERNS=("CONSTANT" "BURST" "RAMP_UP")
REQUESTS=(100 500)
DURATIONS=(10 30)
REPEATS=3

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

        echo "Running $name against node1"
        curl -s -X POST http://node1:8080/api/benchmark/run \
          -H "Content-Type: application/json" \
          -d "$cfg" -o "$jsonf"

        echo "Exporting CSV for $name"
        curl -s -X POST http://node1:8080/api/benchmark/export \
          -H "Content-Type: application/json" \
          -d @"$jsonf" -o "$csvf"

        # extract per-request latencies if available
        latcsv="$OUTDIR/${name}_latencies.csv"
        if [ -f "$jsonf" ]; then
          jq -r '.[] | .latencies | to_entries[] | [.key, .value] | @csv' "$jsonf" > "$latcsv" || true
        fi

        # append to aggregate
        if [ ! -f "$OUTDIR/aggregate.csv" ]; then
          cat "$csvf" > "$OUTDIR/aggregate.csv"
        else
          tail -n +2 "$csvf" >> "$OUTDIR/aggregate.csv"
        fi

      done
    done
  done
done

echo "Sweep complete. Results: $OUTDIR"
