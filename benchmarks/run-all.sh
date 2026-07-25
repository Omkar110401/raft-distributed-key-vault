#!/usr/bin/env bash
set -euo pipefail

# Simple runner: build cluster, run one benchmark, export CSV, tear down.
TS=$(date -u +%Y%m%dT%H%M%SZ)
OUTDIR=benchmarks/results/$TS
mkdir -p "$OUTDIR"

echo "Building and starting cluster (node1:8080, node2:8081, node3:8082)"
docker-compose up -d --build

echo "Waiting for nodes to start..."
sleep 12

CONFIG='{"pattern":"CONSTANT","totalRequests":100,"durationSeconds":10,"burstSize":10,"rampSteps":5}'

echo "Running benchmark against node1"
curl -s -X POST http://localhost:8080/api/benchmark/run \
  -H "Content-Type: application/json" \
  -d "$CONFIG" -o $OUTDIR/results.json

echo "Exporting CSV"
curl -s -X POST http://localhost:8080/api/benchmark/export \
  -H "Content-Type: application/json" \
  -d @${OUTDIR}/results.json -o ${OUTDIR}/results.csv

echo "Results written to ${OUTDIR}"

echo "Shutting down cluster"
docker-compose down

echo "Done"
